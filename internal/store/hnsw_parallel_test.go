package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateTestVectors creates n random test vectors of given dimensions
func generateTestVectors(n, dims int) [][]float32 {
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = float32(i*dims + j)
		}
		vectors[i] = vec
	}
	return vectors
}

// TestAddBatchParallel_Basic tests basic parallel batch addition
func TestAddBatchParallel_Basic(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := generateTestVectors(100, 4)
	rec := makeHNSWTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	locations := make([]Location, 100)
	for i := 0; i < 100; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}

	err := idx.AddBatchParallel(locations, 4)
	require.NoError(t, err)
	assert.Equal(t, 100, idx.Len())
}

// TestAddBatchParallel_SingleWorker tests with single worker (sequential)
func TestAddBatchParallel_SingleWorker(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := generateTestVectors(50, 4)
	rec := makeHNSWTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	locations := make([]Location, 50)
	for i := 0; i < 50; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}

	err := idx.AddBatchParallel(locations, 1)
	require.NoError(t, err)
	assert.Equal(t, 50, idx.Len())
}

// TestAddBatchParallel_EmptyBatch tests with empty batch
func TestAddBatchParallel_EmptyBatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := generateTestVectors(10, 4)
	rec := makeHNSWTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	locations := []Location{}
	err := idx.AddBatchParallel(locations, 4)
	require.NoError(t, err)
	assert.Equal(t, 0, idx.Len())
}

// TestAddBatchParallel_ConcurrentSafety tests thread safety
func TestAddBatchParallel_ConcurrentSafety(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := generateTestVectors(200, 4)
	rec := makeHNSWTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	locations := make([]Location, 200)
	for i := 0; i < 200; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}

	// Run multiple parallel batches concurrently
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			batch := locations[start*50 : (start+1)*50]
			_ = idx.AddBatchParallel(batch, 2)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 200, idx.Len())
}

// TestAddBatchParallel_SearchQuality tests search results after parallel insertion
func TestAddBatchParallel_SearchQuality(t *testing.T) {
	mem := memory.NewGoAllocator()
	// Create sequential vectors where neighbors should be close
	vectors := make([][]float32, 50)
	for i := 0; i < 50; i++ {
		vectors[i] = []float32{float32(i), float32(i), float32(i), float32(i)}
	}
	rec := makeHNSWTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	locations := make([]Location, 50)
	for i := 0; i < 50; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}

	err := idx.AddBatchParallel(locations, 4)
	require.NoError(t, err)

	// Search for vector 25, neighbors should be close indices
	result := idx.SearchByID(25, 3)
	require.NotNil(t, result)
	require.GreaterOrEqual(t, len(result), 3)

	// First result should be self
	assert.Equal(t, VectorID(25), result[0])

	// Other results should be reasonably close (HNSW is approximate)
	for _, r := range result[1:] {
		diff := int(r) - 25
		if diff < 0 {
			diff = -diff
		}
		assert.LessOrEqual(t, diff, 25, "neighbors should be within half dataset size for HNSW approximate search")
	}
}

// TestAddBatchParallel_WorkerCounts tests various worker configurations
func TestAddBatchParallel_WorkerCounts(t *testing.T) {
	workerCounts := []int{1, 2, 4, 8, 16}

	for _, workers := range workerCounts {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			mem := memory.NewGoAllocator()
			vectors := generateTestVectors(100, 4)
			rec := makeHNSWTestRecord(mem, 4, vectors)
			defer rec.Release()

			ds := &Dataset{
				Records: []arrow.RecordBatch{rec},
			}
			idx := NewHNSWIndex(ds)

			locations := make([]Location, 100)
			for i := 0; i < 100; i++ {
				locations[i] = Location{BatchIdx: 0, RowIdx: i}
			}

			err := idx.AddBatchParallel(locations, workers)
			require.NoError(t, err)
			assert.Equal(t, 100, idx.Len())
		})
	}
}

// TestAddBatchParallel_LargeScale tests with larger dataset
func TestAddBatchParallel_LargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large scale test in short mode")
	}

	mem := memory.NewGoAllocator()
	numVectors := 10000
	vectors := generateTestVectors(numVectors, 128)
	rec := makeHNSWTestRecord(mem, 128, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	locations := make([]Location, numVectors)
	for i := 0; i < numVectors; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}

	start := time.Now()
	err := idx.AddBatchParallel(locations, 8)
	duration := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, numVectors, idx.Len())
	t.Logf("Added %d vectors in %v with %d workers", numVectors, duration, 8)
}

// BenchmarkAddBatchParallel benchmarks parallel vs sequential insertion
func BenchmarkAddBatchParallel(b *testing.B) {
	mem := memory.NewGoAllocator()
	numVectors := 1000
	vectors := generateTestVectors(numVectors, 64)
	rec := makeHNSWTestRecord(mem, 64, vectors)
	defer rec.Release()

	locations := make([]Location, numVectors)
	for i := 0; i < numVectors; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}

	b.Run("Sequential", func(b *testing.B) {
		b.SetBytes(int64(numVectors * 64 * 4))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ds := &Dataset{
				Records: []arrow.RecordBatch{rec},
			}
			idx := NewHNSWIndex(ds)
			for j := 0; j < numVectors; j++ {
				_, _ = idx.Add(0, j)
			}
		}
		b.ReportMetric(float64(numVectors*b.N)/b.Elapsed().Seconds(), "vectors/sec")
	})

	for _, workers := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("Parallel_%d_workers", workers), func(b *testing.B) {
			b.SetBytes(int64(numVectors * 64 * 4))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ds := &Dataset{
					Records: []arrow.RecordBatch{rec},
				}
				idx := NewHNSWIndex(ds)
				_ = idx.AddBatchParallel(locations, workers)
			}
			b.ReportMetric(float64(numVectors*b.N)/b.Elapsed().Seconds(), "vectors/sec")
		})
	}
}
