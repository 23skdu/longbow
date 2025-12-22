package store

import (
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	_ "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createZeroCopyTestIndex creates an HNSW index for zero-copy tests
func createZeroCopyTestIndex(t *testing.T, dims, count int) *HNSWIndex {
	t.Helper()
	mem := memory.NewGoAllocator()

	vectors := make([][]float32, count)
	for i := 0; i < count; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = float32(i*dims + j)
		}
		vectors[i] = vec
	}

	rec := makeHNSWTestRecord(mem, dims, vectors)
	ds := &Dataset{
		Name:    "zerocopy-test",
		Records: []arrow.RecordBatch{rec},
	}

	idx := NewHNSWIndex(ds)
	for i := 0; i < count; i++ {
		_, err := idx.Add(0, i)
		require.NoError(t, err)
	}

	return idx
}

// createZeroCopyBenchIndex creates an HNSW index for benchmarks
func createZeroCopyBenchIndex(b *testing.B, dims, count int) *HNSWIndex {
	b.Helper()
	mem := memory.NewGoAllocator()

	vectors := make([][]float32, count)
	for i := 0; i < count; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = float32(i*dims + j)
		}
		vectors[i] = vec
	}

	rec := makeHNSWTestRecord(mem, dims, vectors)
	ds := &Dataset{
		Name:    "zerocopy-bench",
		Records: []arrow.RecordBatch{rec},
	}

	idx := NewHNSWIndex(ds)
	for i := 0; i < count; i++ {
		_, _ = idx.Add(0, i)
	}

	runtime.GC()
	return idx
}

// TestSearchByIDUnsafe verifies zero-copy search returns correct results
func TestSearchByIDUnsafe(t *testing.T) {
	idx := createZeroCopyTestIndex(t, 64, 10)

	results := idx.SearchByIDUnsafe(0, 5)
	require.NotNil(t, results, "SearchByIDUnsafe returned nil")
	assert.NotEmpty(t, results, "SearchByIDUnsafe returned empty")
	assert.Equal(t, VectorID(0), results[0], "first result should be query vector")
}

// TestSearchByIDUnsafeEpochProtection verifies epoch management
func TestSearchByIDUnsafeEpochProtection(t *testing.T) {
	idx := createZeroCopyTestIndex(t, 64, 10)

	initial := idx.activeReaders.Load()
	_ = idx.SearchByIDUnsafe(0, 5)
	final := idx.activeReaders.Load()

	assert.Equal(t, initial, final, "epoch leak detected")
}

// TestSearchByIDUnsafeInvalidID verifies nil for invalid IDs
func TestSearchByIDUnsafeInvalidID(t *testing.T) {
	idx := createZeroCopyTestIndex(t, 64, 10)
	results := idx.SearchByIDUnsafe(9999, 5)
	assert.Nil(t, results, "expected nil for invalid ID")
}

// TestSearchByIDUnsafeZeroK verifies nil for k=0
func TestSearchByIDUnsafeZeroK(t *testing.T) {
	idx := createZeroCopyTestIndex(t, 64, 10)
	results := idx.SearchByIDUnsafe(0, 0)
	assert.Nil(t, results, "expected nil for k=0")
}

// TestSearchByIDUnsafeConcurrent verifies thread safety
func TestSearchByIDUnsafeConcurrent(t *testing.T) {
	idx := createZeroCopyTestIndex(t, 32, 100)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				results := idx.SearchByIDUnsafe(VectorID(id), 5)
				assert.NotNil(t, results)
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int32(0), idx.activeReaders.Load(), "epoch leak after concurrent")
}

// TestSearchByIDUnsafeMatchesSafe verifies results match safe version
func TestSearchByIDUnsafeMatchesSafe(t *testing.T) {
	idx := createZeroCopyTestIndex(t, 256, 50)

	for id := 0; id < 10; id++ {
		safeRes := idx.SearchByID(VectorID(id), 10)
		unsafeRes := idx.SearchByIDUnsafe(VectorID(id), 10)

		require.Equal(t, len(safeRes), len(unsafeRes), "length mismatch id=%d", id)
		for i := range safeRes {
			assert.Equal(t, safeRes[i], unsafeRes[i], "mismatch id=%d pos=%d", id, i)
		}
	}
}

// TestGetVectorZeroCopyDataMatch verifies unsafe data matches safe
func TestGetVectorZeroCopyDataMatch(t *testing.T) {
	idx := createZeroCopyTestIndex(t, 64, 10)

	for id := 0; id < 10; id++ {
		safeVec := idx.getVector(VectorID(id))
		unsafeVec, release := idx.getVectorUnsafe(VectorID(id))
		require.NotNil(t, release, "nil release for id=%d", id)

		require.Equal(t, len(safeVec), len(unsafeVec), "length mismatch id=%d", id)
		for i := range safeVec {
			assert.Equal(t, safeVec[i], unsafeVec[i], "data mismatch id=%d pos=%d", id, i)
		}
		release()
	}
}

// BenchmarkSearchByIDSafe benchmarks copying version
func BenchmarkSearchByIDSafe(b *testing.B) {
	idx := createZeroCopyBenchIndex(b, 64, 500)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = idx.SearchByID(VectorID(i%1000), 10)
	}
}

// BenchmarkSearchByIDUnsafe benchmarks zero-copy version
func BenchmarkSearchByIDUnsafe(b *testing.B) {
	idx := createZeroCopyBenchIndex(b, 256, 2000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = idx.SearchByIDUnsafe(VectorID(i%1000), 10)
	}
}

// BenchmarkSearchByIDSafeAllocs measures safe allocations
func BenchmarkSearchByIDSafeAllocs(b *testing.B) {
	idx := createZeroCopyBenchIndex(b, 128, 1000)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = idx.SearchByID(VectorID(i%1000), 10)
	}
}

// BenchmarkSearchByIDUnsafeAllocs measures zero-copy allocations
func BenchmarkSearchByIDUnsafeAllocs(b *testing.B) {
	idx := createZeroCopyBenchIndex(b, 128, 1000)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = idx.SearchByIDUnsafe(VectorID(i%1000), 10)
	}
}
